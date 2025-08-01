Option Explicit

Dim tbl, col
Dim output

output = "Inspecting all columns in the Physical Model:" & vbCrLf & vbCrLf

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        output = output & "Table: " & tbl.Code & vbCrLf
        For Each col In tbl.Columns
            output = output & "  Column: " & col.Code & vbCrLf
            output = output & "    DataType: '" & col.DataType & "'" & vbCrLf
            output = output & "    Length: " & col.Length & vbCrLf
            
            ' Check if LengthSemantics is available and print its value
            On Error Resume Next
            Dim colSemantics
            colSemantics = col.LengthSemantics
            If Err.Number <> 0 Then
                output = output & "    LengthSemantics: [N/A]" & vbCrLf
                Err.Clear
            Else
                output = output & "    LengthSemantics: '" & col.LengthSemantics & "'" & vbCrLf
            End If
            On Error GoTo 0
        Next
        output = output & vbCrLf
    End If
Next

' Show the detailed debug output
MsgBox output, vbOKOnly, "DEBUG: Column DataTypes and Semantics"


Option Explicit

Dim tbl, col
Dim output

output = "Inspecting all VARCHAR2 columns in the Physical Model:" & vbCrLf & vbCrLf

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        output = output & "Table: " & tbl.Code & vbCrLf
        For Each col In tbl.Columns
            If UCase(col.DataType) = "VARCHAR2" Then
                output = output & "  Column: " & col.Code & vbCrLf
                output = output & "    DataType: '" & col.DataType & "'" & vbCrLf
                output = output & "    Length: " & col.Length & vbCrLf
            End If
        Next
    End If
Next

MsgBox output, vbOKOnly, "DEBUG: VARCHAR2 Columns"
